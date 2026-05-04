import{ useState } from 'react';
import { useNavigate } from 'react-router-dom';

const Login = () => {
  // 1. State management for the form
  const [formData, setFormData] = useState({
    email: '',
    password: ''
  });
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const navigate = useNavigate();

  // 2. Handle input changes dynamically
  const handleChange = (e) => {
    const { name, value } = e.target;
    setFormData(prev => ({
      ...prev,
      [name]: value
    }));
  };

  // 3. Handle Form Submission
  const handleLogin = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    // Basic Validation
    if (!formData.email || !formData.password) {
      setError('Please enter both email and password.');
      setLoading(false);
      return;
    }

    // Mock Authentication Logic
    // Replace this with your actual API call: axios.post('/api/auth', formData)
    setTimeout(() => {
      if (formData.email === 'admin@erp.com' && formData.password === 'password123') {
        console.log('Login Successful');
        localStorage.setItem('isAuthenticated', 'true');
        navigate('/sales'); 
      } else {
        setError('Invalid credentials. Try admin@erp.com / password123');
      }
      setLoading(false);
    }, 1000); // Simulating network delay
  };

  return (
    <div className="login-page">
      <div className="login-card">
        <h1>Insighto</h1>
        <p className="subtitle">Enter your credentials</p>
        
        {/* Error Alert */}
        {error && <div className="error-message">{error}</div>}

        <form onSubmit={handleLogin}>
          <div className="login-field">
            <label htmlFor="email">Email Address</label>
            <input 
              id="email"
              name="email"
              type="email" 
              placeholder="name@company.com" 
              value={formData.email}
              onChange={handleChange}
              required
            />
          </div>
          
          <div className="login-field">
            <label htmlFor="password">Password</label>
            <input 
              id="password"
              name="password"
              type="password" 
              placeholder="••••••••" 
              value={formData.password}
              onChange={handleChange}
              required
            />
          </div>
          
          <button 
            type="submit" 
            className="btn-login" 
            disabled={loading}
          >
            {loading ? 'Authenticating...' : 'Login'}
          </button>
        </form>
        
        <div className="login-footer">
          <span className="forgot">Forgot Password?</span>
        </div>
      </div>
    </div>
  );
};

export default Login;